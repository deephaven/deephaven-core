//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.DurationType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.GenericVectorType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LocalDateType;
import io.deephaven.qst.type.LocalTimeType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.NativeArrayType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.PrimitiveVectorType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.vector.ByteVector;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.DoubleVector;
import io.deephaven.vector.FloatVector;
import io.deephaven.vector.IntVector;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ShortVector;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Column definition for all Deephaven columns.
 *
 * <p>
 * Every column definition has a valid column name: each factory method throws
 * {@link NameValidator.InvalidNameException} (an {@link IllegalArgumentException}) if the name is not, as determined by
 * {@link NameValidator#validateColumnName(String)}.
 */
public class ColumnDefinition<TYPE> implements LogOutputAppendable {

    public static final ColumnDefinition<?>[] ZERO_LENGTH_COLUMN_DEFINITION_ARRAY = new ColumnDefinition[0];

    public enum ColumnType {
        /**
         * A normal column, with no special considerations.
         */
        Normal,

        /**
         * A column that helps define underlying partitions in the storage of the data, which consequently may also be
         * used for very efficient filtering.
         */
        Partitioning
    }

    /**
     * Creates a {@link ColumnType#Normal normal} boolean column definition, with data type {@link Boolean}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<Boolean> ofBoolean(@NotNull final String name) {
        return create(name, Boolean.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} byte column definition, with data type {@code byte}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<Byte> ofByte(@NotNull final String name) {
        return create(name, byte.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} char column definition, with data type {@code char}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<Character> ofChar(@NotNull final String name) {
        return create(name, char.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} short column definition, with data type {@code short}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<Short> ofShort(@NotNull final String name) {
        return create(name, short.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} int column definition, with data type {@code int}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<Integer> ofInt(@NotNull final String name) {
        return create(name, int.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} long column definition, with data type {@code long}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<Long> ofLong(@NotNull final String name) {
        return create(name, long.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} float column definition, with data type {@code float}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<Float> ofFloat(@NotNull final String name) {
        return create(name, float.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} double column definition, with data type {@code double}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<Double> ofDouble(@NotNull final String name) {
        return create(name, double.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} string column definition, with data type {@link String}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<String> ofString(@NotNull final String name) {
        return create(name, String.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} timestamp column definition, with data type {@link Instant}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<Instant> ofTime(@NotNull final String name) {
        return create(name, Instant.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} time-of-day column definition, with data type {@link LocalTime}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<LocalTime> ofLocalTime(@NotNull final String name) {
        return create(name, LocalTime.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} date column definition, with data type {@link LocalDate}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<LocalDate> ofLocalDate(@NotNull final String name) {
        return create(name, LocalDate.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} duration column definition, with data type {@link Duration}.
     *
     * @param name the column name
     * @return the column definition
     */
    public static ColumnDefinition<Duration> ofDuration(@NotNull final String name) {
        return create(name, Duration.class);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} column definition for a qst {@link Type}. The data type is:
     * <ul>
     * <li>for {@link PrimitiveType primitive types}, the primitive type, except {@link Boolean} for
     * {@link BooleanType};</li>
     * <li>for {@link BoxedType boxed types}, the same as for the corresponding primitive type;</li>
     * <li>for other {@link GenericType generic types}, the type's class, with the array or {@link Vector} component
     * type as the component type for {@link ArrayType array types}. A {@link CustomType} whose class is a
     * {@link Vector} implementation (outside {@code io.deephaven.vector}, which CustomType does not allow) gets an
     * inferred component type, as by {@link #fromGenericType(String, Class)}.</li>
     * </ul>
     *
     * @param name the column name
     * @param type the type
     * @return the column definition
     * @throws IllegalArgumentException if {@code type} is a {@link CustomType} for {@code void} or {@link Void}
     */
    public static ColumnDefinition<?> of(String name, Type<?> type) {
        return type.walk(new Adapter(name));
    }

    /**
     * Creates a {@link ColumnType#Normal normal} column definition for a qst {@link PrimitiveType}. The data type is
     * the primitive type, except {@link Boolean} for {@link BooleanType}.
     *
     * @param name the column name
     * @param type the type
     * @return the column definition
     */
    public static ColumnDefinition<?> of(String name, PrimitiveType<?> type) {
        return type.walk((PrimitiveType.Visitor<ColumnDefinition<?>>) new Adapter(name));
    }

    /**
     * Creates a {@link ColumnType#Normal normal} column definition for a qst {@link GenericType}. The data type is the
     * same as for the corresponding primitive type for {@link BoxedType boxed types}, and otherwise the type's class,
     * with the array or {@link Vector} component type as the component type for {@link ArrayType array types}. A
     * {@link CustomType} whose class is a {@link Vector} implementation gets an inferred component type, as by
     * {@link #fromGenericType(String, Class)}.
     *
     * @param name the column name
     * @param type the type
     * @return the column definition
     * @throws IllegalArgumentException if {@code type} is a {@link CustomType} for {@code void} or {@link Void}
     */
    public static ColumnDefinition<?> of(String name, GenericType<?> type) {
        return type.walk((GenericType.Visitor<ColumnDefinition<?>>) new Adapter(name));
    }

    /**
     * Creates a {@link ColumnType#Normal normal} column definition for a qst {@link ArrayType}: a native array or
     * {@link Vector} type. The data type is the type's class, and the component type is the type's component type.
     *
     * @param name the column name
     * @param type the type
     * @return the column definition
     */
    public static <T> ColumnDefinition<T> of(String name, ArrayType<T, ?> type) {
        return type.walk(new ArrayAdapter<>(name));
    }

    /**
     * Creates a {@link ColumnType#Normal normal} column definition for a qst {@link PrimitiveVectorType}, such as
     * {@link IntVector}. The data type is the Vector type, and the component type is its primitive component type.
     *
     * @param name the column name
     * @param type the type
     * @return the column definition
     */
    public static <T> ColumnDefinition<T> of(String name, PrimitiveVectorType<T, ?> type) {
        return create(name, type.clazz(), type.componentType().clazz(), ColumnType.Normal);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} column definition for a qst {@link GenericVectorType}, an
     * {@link ObjectVector} of a generic type. The data type is {@link ObjectVector}, and the {@link #getComponentType()
     * component type} is the class of the type's component type, which supplies the element type that
     * {@link ObjectVector} only expresses through generics: {@code ObjectVector<Number>} has component type
     * {@link Number}, making it the Vector equivalent of {@code Number[]}.
     *
     * @param name the column name
     * @param type the type
     * @return the column definition
     */
    public static <T> ColumnDefinition<T> of(String name, GenericVectorType<T, ?> type) {
        return create(name, type.clazz(), type.componentType().clazz(), ColumnType.Normal);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} column definition for a qst {@link NativeArrayType}. The data type is
     * the array type, and the component type is the class of its component type.
     *
     * @param name the column name
     * @param type the type
     * @return the column definition
     */
    public static <T> ColumnDefinition<T> of(String name, NativeArrayType<T, ?> type) {
        return create(name, type.clazz(), type.componentType().clazz(), ColumnType.Normal);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} {@link Vector} column definition. The data type is {@code vectorType},
     * and the {@link #getComponentType() component type} is its element type: the primitive type for primitive Vectors,
     * such as {@code int} for {@link IntVector} (the Vector equivalent of {@code int[]}); and {@link Object} for
     * {@link ObjectVector}, making it {@code ObjectVector<Object>}, the Vector equivalent of {@code Object[]}. To give
     * an {@link ObjectVector} a more specific element type, use {@link #ofVector(String, Class, Class)}.
     *
     * @param name the column name
     * @param vectorType the Vector type
     * @return the column definition
     * @throws IllegalArgumentException if {@code vectorType} is not a recognized Vector type
     */
    public static <T extends Vector<?>> ColumnDefinition<T> ofVector(
            @NotNull final String name,
            @NotNull final Class<T> vectorType) {
        return create(name, vectorType, baseComponentTypeForVector(vectorType), ColumnType.Normal);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} {@link Vector} column definition. The data type is {@code vectorType},
     * and the {@link #getComponentType() component type} is its element type.
     *
     * <p>
     * {@link ObjectVector} expresses its element type only through generics, so {@code componentType} supplies it:
     * {@code ofVector(name, ObjectVector.class, Number.class)} describes {@code ObjectVector<Number>}, the Vector
     * equivalent of {@code Number[]}. Primitive Vectors fix their element type, so for them {@code componentType} must
     * be that primitive type, such as {@code int} for {@link IntVector}.
     *
     * @param name the column name
     * @param vectorType the Vector type
     * @param componentType the element type: any reference type but {@link Void} for {@link ObjectVector}, or the
     *        primitive type for a primitive Vector; or {@code null} for the default, which is {@link Object} for
     *        {@link ObjectVector}
     * @return the column definition
     * @throws IllegalArgumentException if {@code vectorType} is not a recognized Vector type, or {@code componentType}
     *         is not valid for it
     */
    public static <T extends Vector<?>> ColumnDefinition<T> ofVector(
            @NotNull final String name,
            @NotNull final Class<T> vectorType,
            @Nullable final Class<?> componentType) {
        return create(name, vectorType, inferComponentType(vectorType, componentType), ColumnType.Normal);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} column definition for an arbitrary data type, inferring the component
     * type. Equivalent to {@code fromGenericType(name, dataType, null, ColumnType.Normal)}; see
     * {@link #fromGenericType(String, Class, Class, ColumnType)} for how the data and component types are determined.
     *
     * @param name the column name
     * @param dataType the data type
     * @return the column definition
     * @throws IllegalArgumentException if {@code dataType} is not a valid column data type
     */
    public static <T> ColumnDefinition<T> fromGenericType(
            @NotNull final String name,
            @NotNull final Class<T> dataType) {
        return fromGenericType(name, dataType, null);
    }

    /**
     * Creates a {@link ColumnType#Normal normal} column definition for an arbitrary data type. Equivalent to
     * {@code fromGenericType(name, dataType, componentType, ColumnType.Normal)}; see
     * {@link #fromGenericType(String, Class, Class, ColumnType)} for how the data and component types are determined.
     *
     * @param name the column name
     * @param dataType the data type
     * @param componentType the component type, for array and {@link Vector} data types; inferred when {@code null}
     * @return the column definition
     * @throws IllegalArgumentException if {@code dataType} is not a valid column data type, or {@code componentType} is
     *         not valid for {@code dataType}
     */
    public static <T> ColumnDefinition<T> fromGenericType(
            @NotNull final String name,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType) {
        return fromGenericType(name, dataType, componentType, ColumnType.Normal);
    }

    /**
     * Creates a column definition for an arbitrary data type.
     *
     * <p>
     * Like column sources, column definitions use {@link Boolean} for boolean columns and the primitive type for other
     * primitive columns, so {@code dataType} is normalized: {@code boolean} becomes {@link Boolean}, and the other
     * boxed primitive types become their primitive types. Other data types are used as is.
     *
     * <p>
     * For array and {@link Vector} data types, the {@link #getComponentType() component type} is the element type. When
     * {@code componentType} is {@code null}, it is inferred: the array's component type for array data types; the
     * primitive type for primitive Vectors, such as {@code int} for {@link IntVector}; and {@link Object} for
     * {@link ObjectVector}. Since {@link ObjectVector} expresses its element type only through generics, give
     * {@code componentType} to describe, for example, {@code ObjectVector<Number>}, the Vector equivalent of
     * {@code Number[]}. Other data types have no component type unless one is given.
     *
     * @param name the column name
     * @param dataType the data type; may not be {@code void} or {@link Void}, nor an array of {@link Void}
     * @param componentType the element type for array and {@link Vector} data types: for arrays, the array's component
     *        type or a more specific one; for {@link ObjectVector}, any reference type; for primitive Vectors, their
     *        primitive type. Never {@code void} or {@link Void}, nor an array of them. Or {@code null} to infer it
     * @param columnType the column type
     * @return the column definition
     * @throws IllegalArgumentException if {@code dataType} is not a valid column data type, or {@code componentType} is
     *         not valid for {@code dataType}
     */
    public static <T> ColumnDefinition<T> fromGenericType(
            @NotNull final String name,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType,
            @NotNull final ColumnType columnType) {
        final Class<T> normalizedDataType = normalizeDataType(Objects.requireNonNull(dataType));
        return create(name, normalizedDataType, inferComponentType(normalizedDataType, componentType), columnType);
    }

    /**
     * Boxed primitive types, mapped to their primitive types. Column definitions use the primitive type rather than the
     * boxed type; {@link Boolean} is not included, as it is the data type for boolean columns.
     */
    private static final Map<Class<?>, Class<?>> BOXED_PRIMITIVE_TYPES = Map.of(
            Byte.class, byte.class,
            Character.class, char.class,
            Short.class, short.class,
            Integer.class, int.class,
            Long.class, long.class,
            Float.class, float.class,
            Double.class, double.class);

    /**
     * Describes why {@code dataType} is not a valid column data type, or returns {@code null} if it is valid.
     * {@code void} and {@link Void} are never valid. Boolean columns use {@link Boolean} rather than {@code boolean},
     * and the other primitive columns use their primitive type rather than its boxed type.
     */
    @Nullable
    private static String dataTypeError(@NotNull final String name, @NotNull final Class<?> dataType) {
        if (dataType == void.class || dataType == Void.class) {
            return "Invalid data type " + dataType.getTypeName() + " for column " + name + ": not a valid column type";
        }
        if (dataType == boolean.class) {
            return "Invalid data type boolean for column " + name + ": use " + Boolean.class.getName();
        }
        final Class<?> primitiveType = BOXED_PRIMITIVE_TYPES.get(dataType);
        if (primitiveType != null) {
            return "Invalid data type " + dataType.getTypeName() + " for column " + name + ": use " + primitiveType;
        }
        return null;
    }

    /**
     * Normalizes {@code dataType} to the type column definitions use for it: {@link Boolean} for {@code boolean}, and
     * the primitive type for the other boxed primitive types. Other types are returned unchanged.
     */
    private static <T> Class<T> normalizeDataType(@NotNull final Class<T> dataType) {
        if (dataType == boolean.class) {
            // noinspection unchecked
            return (Class<T>) Boolean.class;
        }
        final Class<?> primitiveType = BOXED_PRIMITIVE_TYPES.get(dataType);
        // noinspection unchecked
        return primitiveType != null ? (Class<T>) primitiveType : dataType;
    }

    /**
     * Base component type class for each {@link Vector} type.
     *
     * @throws IllegalArgumentException if {@code vectorType} is not a recognized Vector type
     */
    private static Class<?> baseComponentTypeForVector(@NotNull final Class<? extends Vector<?>> vectorType) {
        final Class<?> baseComponentType = findBaseComponentTypeForVector(vectorType);
        if (baseComponentType == null) {
            throw new IllegalArgumentException("Unrecognized Vector type " + vectorType.getTypeName());
        }
        return baseComponentType;
    }

    /**
     * Base component type class for each {@link Vector} type, or {@code null} if {@code vectorType} is not a recognized
     * Vector type.
     */
    @Nullable
    private static Class<?> findBaseComponentTypeForVector(@NotNull final Class<? extends Vector<?>> vectorType) {
        if (CharVector.class.isAssignableFrom(vectorType)) {
            return char.class;
        }
        if (ByteVector.class.isAssignableFrom(vectorType)) {
            return byte.class;
        }
        if (ShortVector.class.isAssignableFrom(vectorType)) {
            return short.class;
        }
        if (IntVector.class.isAssignableFrom(vectorType)) {
            return int.class;
        }
        if (LongVector.class.isAssignableFrom(vectorType)) {
            return long.class;
        }
        if (FloatVector.class.isAssignableFrom(vectorType)) {
            return float.class;
        }
        if (DoubleVector.class.isAssignableFrom(vectorType)) {
            return double.class;
        }
        if (ObjectVector.class.isAssignableFrom(vectorType)) {
            return Object.class;
        }
        return null;
    }

    /**
     * Infers the component type for {@code dataType} when {@code componentType} is not given: the array component type
     * for arrays, and the {@link #baseComponentTypeForVector(Class) base component type} for {@link Vector Vectors}
     * (the primitive type for primitive Vectors, and {@link Object} for {@link ObjectVector}, making it
     * {@code ObjectVector<Object>}). This does not check that a given {@code componentType} is valid;
     * {@link #create(String, Class, Class, ColumnType)} does.
     */
    @Nullable
    private static Class<?> inferComponentType(
            @NotNull final Class<?> dataType,
            @Nullable final Class<?> componentType) {
        if (componentType != null) {
            return componentType;
        }
        if (dataType.isArray()) {
            return dataType.getComponentType();
        }
        if (Vector.class.isAssignableFrom(dataType)) {
            /*
             * TODO (https://github.com/deephaven/deephaven-core/issues/817): Allow formula results returning Vector to
             * know component type, and then require it rather than inferring it.
             */
            // noinspection unchecked
            return baseComponentTypeForVector((Class<? extends Vector<?>>) dataType);
        }
        return null;
    }

    /**
     * Describes why {@code componentType} is not valid for {@code dataType}, or returns {@code null} if it is valid.
     * Array and {@link Vector} data types require a {@link #getComponentType() component type}: for arrays, the array's
     * component type or a more specific one; for Vectors, a type assignable to the Vector's
     * {@link #baseComponentTypeForVector(Class) base component type}, which allows any reference type as the element
     * type of an {@link ObjectVector}, and only the matching primitive type for a primitive Vector. No component type
     * may be {@code void} or {@link Void}, or an array of them, whatever the data type.
     */
    @Nullable
    private static String componentTypeError(
            @NotNull final String name,
            @NotNull final Class<?> dataType,
            @Nullable final Class<?> componentType) {
        if (componentType != null && isVoidElementType(componentType)) {
            return "Invalid component type " + componentType.getTypeName() + " for column " + name
                    + ": void and Void are not valid element types";
        }
        final Class<?> requiredComponentType;
        final String kind;
        if (dataType.isArray()) {
            requiredComponentType = dataType.getComponentType();
            kind = "array";
        } else if (Vector.class.isAssignableFrom(dataType)) {
            // noinspection unchecked
            requiredComponentType = findBaseComponentTypeForVector((Class<? extends Vector<?>>) dataType);
            if (requiredComponentType == null) {
                return "Unrecognized Vector type " + dataType.getTypeName() + " for column " + name;
            }
            kind = "Vector";
        } else {
            // Note: some testing currently depends on being able to create Collection + componentType definitions:
            // io.deephaven.server.jetty.BarrageChunkFactoryTest.testNotAListDestinationPropagation
            // if (componentType != null) {
            // return String.format(
            // "Invalid componentType %s for non-array, non-Vector dataType %s", componentType, dataType);
            // }
            return null;
        }
        if (componentType == null) {
            return "Missing component type for column " + name + " with " + kind + " data type "
                    + dataType.getTypeName();
        }
        if (!requiredComponentType.isAssignableFrom(componentType)) {
            return "Invalid component type " + componentType.getTypeName() + " for column " + name + " with " + kind
                    + " data type " + dataType.getTypeName();
        }
        return null;
    }

    /**
     * Whether {@code type} is {@code void} or {@link Void}, or an array (of any depth) whose innermost element type is.
     * Neither can hold any values but {@code null}, so neither is a valid element type for a column.
     */
    private static boolean isVoidElementType(@NotNull Class<?> type) {
        while (type.isArray()) {
            type = type.getComponentType();
        }
        return type == void.class || type == Void.class;
    }

    /**
     * Throws an {@link IllegalArgumentException} with {@code error}, if there is one.
     */
    private static void checkArgument(@Nullable final String error) {
        if (error != null) {
            throw new IllegalArgumentException(error);
        }
    }

    /**
     * Creates a {@link ColumnType#Normal normal} column definition from a qst {@link ColumnHeader}. The name is the
     * header's name, and the data type is determined from the header's type as by {@link #of(String, Type)}.
     *
     * @param header the column header
     * @return the column definition
     * @throws IllegalArgumentException if the header's type is a {@link CustomType} for {@code void} or {@link Void}
     */
    public static ColumnDefinition<?> from(ColumnHeader<?> header) {
        return header.componentType().walk(new Adapter(header.name()));
    }

    private static class Adapter implements Type.Visitor<ColumnDefinition<?>>,
            PrimitiveType.Visitor<ColumnDefinition<?>>, GenericType.Visitor<ColumnDefinition<?>> {

        private final String name;

        public Adapter(String name) {
            this.name = Objects.requireNonNull(name);
        }

        @Override
        public ColumnDefinition<?> visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<ColumnDefinition<?>>) this);
        }

        @Override
        public ColumnDefinition<?> visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<ColumnDefinition<?>>) this);
        }

        @Override
        public ColumnDefinition<?> visit(BooleanType booleanType) {
            return ofBoolean(name);
        }

        @Override
        public ColumnDefinition<?> visit(ByteType byteType) {
            return ofByte(name);
        }

        @Override
        public ColumnDefinition<?> visit(CharType charType) {
            return ofChar(name);
        }

        @Override
        public ColumnDefinition<?> visit(ShortType shortType) {
            return ofShort(name);
        }

        @Override
        public ColumnDefinition<?> visit(IntType intType) {
            return ofInt(name);
        }

        @Override
        public ColumnDefinition<?> visit(LongType longType) {
            return ofLong(name);
        }

        @Override
        public ColumnDefinition<?> visit(FloatType floatType) {
            return ofFloat(name);
        }

        @Override
        public ColumnDefinition<?> visit(DoubleType doubleType) {
            return ofDouble(name);
        }

        @Override
        public ColumnDefinition<?> visit(BoxedType<?> boxedType) {
            // treat the same as primitive type
            return visit(boxedType.primitiveType());
        }

        @Override
        public ColumnDefinition<?> visit(StringType stringType) {
            return ofString(name);
        }

        @Override
        public ColumnDefinition<?> visit(InstantType instantType) {
            return ofTime(name);
        }

        @Override
        public ColumnDefinition<?> visit(LocalTimeType localTimeType) {
            return ofLocalTime(name);
        }

        @Override
        public ColumnDefinition<?> visit(LocalDateType localDateType) {
            return ofLocalDate(name);
        }

        @Override
        public ColumnDefinition<?> visit(DurationType durationType) {
            return ofDuration(name);
        }

        @Override
        public ColumnDefinition<?> visit(ArrayType<?, ?> arrayType) {
            return of(name, arrayType);
        }

        @Override
        public ColumnDefinition<?> visit(CustomType<?> customType) {
            // CustomType excludes primitive, boxed, and array types, but only excludes Vector types by package name
            // (io.deephaven.vector), so it may still be a Vector implementation in another package, such as
            // IntVectorColumnWrapper, whose component type must be inferred. It may also be void or Void, which are not
            // static types. fromGenericType infers the component type, and rejects void and Void.
            return fromGenericType(name, customType.clazz());
        }
    }

    private static class ArrayAdapter<T> implements ArrayType.Visitor<ColumnDefinition<T>> {

        private final String name;

        public ArrayAdapter(String name) {
            this.name = Objects.requireNonNull(name);
        }

        @Override
        public ColumnDefinition<T> visit(NativeArrayType<?, ?> nativeArrayType) {
            // noinspection unchecked
            return of(name, (NativeArrayType<T, ?>) nativeArrayType);
        }

        @Override
        public ColumnDefinition<T> visit(PrimitiveVectorType<?, ?> vectorPrimitiveType) {
            // noinspection unchecked
            return of(name, (PrimitiveVectorType<T, ?>) vectorPrimitiveType);
        }

        @Override
        public ColumnDefinition<T> visit(GenericVectorType<?, ?> genericVectorType) {
            // noinspection unchecked
            return of(name, (GenericVectorType<T, ?>) genericVectorType);
        }
    }

    @NotNull
    private final String name;
    @NotNull
    private final Class<TYPE> dataType;
    @Nullable
    private final Class<?> componentType;
    @NotNull
    private final ColumnType columnType;

    /**
     * Creates a {@link ColumnType#Normal normal} column definition with no component type, after checking that it is
     * valid. See {@link #create(String, Class, Class, ColumnType)}.
     */
    private static <T> ColumnDefinition<T> create(@NotNull final String name, @NotNull final Class<T> dataType) {
        return create(name, dataType, null, ColumnType.Normal);
    }

    /**
     * Creates a column definition, after checking that it is valid: that {@code name} is a valid column name, that
     * {@code dataType} is a valid column data type, and that {@code componentType} is valid for {@code dataType}. Any
     * normalization or inference of the types must already have been done.
     *
     * <p>
     * All construction paths that take arguments from callers go through this method. Only paths that already know the
     * result is valid, such as {@link #withPartitioning()}, call the constructor directly, so that they need not repeat
     * the checks.
     *
     * @throws NameValidator.InvalidNameException if {@code name} is not a valid column name
     * @throws IllegalArgumentException if {@code dataType} or {@code componentType} is not valid
     */
    private static <T> ColumnDefinition<T> create(
            @NotNull final String name,
            @NotNull final Class<T> dataType,
            @Nullable final Class<?> componentType,
            @NotNull final ColumnType columnType) {
        NameValidator.validateColumnName(Objects.requireNonNull(name, "Column names cannot be null"));
        Objects.requireNonNull(dataType);
        Objects.requireNonNull(columnType);
        checkArgument(dataTypeError(name, dataType));
        checkArgument(componentTypeError(name, dataType, componentType));
        return new ColumnDefinition<>(name, dataType, componentType, columnType);
    }

    /**
     * Constructs a column definition without any checks. Callers must already know that the result is valid; use
     * {@link #create(String, Class, Class, ColumnType)} otherwise.
     */
    private ColumnDefinition(
            @NotNull final String name,
            @NotNull final Class<TYPE> dataType,
            @Nullable final Class<?> componentType,
            @NotNull final ColumnType columnType) {
        this.name = name;
        this.dataType = dataType;
        this.componentType = componentType;
        this.columnType = columnType;
    }

    /**
     * The column name. It is always a valid column name, as determined by
     * {@link NameValidator#validateColumnName(String)}.
     *
     * @return the column name
     */
    @NotNull
    public String getName() {
        return name;
    }

    /**
     * The data type: the type of the column's values.
     *
     * <p>
     * Like column sources, column definitions use {@link Boolean} for boolean columns, and the primitive type for other
     * primitive columns (such as {@code int}, never {@link Integer}). The data type is never {@code void} or
     * {@link Void}. For array and {@link Vector} data types, the type of the elements is the {@link #getComponentType()
     * component type}.
     *
     * @return the data type
     */
    @NotNull
    public Class<TYPE> getDataType() {
        return dataType;
    }

    /**
     * The component type: for array and {@link Vector} data types, the type of the elements.
     *
     * <p>
     * A native array type already carries its element type, so for an array data type the component type is the array's
     * component type ({@code Number} for {@code Number[]}), or a more specific type that all elements are known to have
     * ({@code Integer} for a {@code Number[]} holding only {@code Integer}s). Primitive Vectors likewise fix their
     * element type: the component type of an {@link IntVector} column is {@code int}, as for {@code int[]}.
     * {@link ObjectVector}, however, expresses its element type only through generics, so the component type supplies
     * it: an {@link ObjectVector} column with component type {@code Number} is an {@code ObjectVector<Number>}, the
     * Vector equivalent of {@code Number[]}, not an {@code Object[]}-like column with a narrower component type.
     *
     * <p>
     * Other data types have no component type, unless one was given when creating the definition.
     *
     * @return the component type, or {@code null} if there is none
     */
    @Nullable
    public Class<?> getComponentType() {
        return componentType;
    }

    @NotNull
    public ColumnType getColumnType() {
        return columnType;
    }

    public ColumnDefinition<TYPE> withPartitioning() {
        return isPartitioning() ? this : new ColumnDefinition<>(name, dataType, componentType, ColumnType.Partitioning);
    }

    public ColumnDefinition<TYPE> withNormal() {
        return columnType == ColumnType.Normal
                ? this
                : new ColumnDefinition<>(name, dataType, componentType, ColumnType.Normal);
    }

    public <Other> ColumnDefinition<Other> withDataType(@NotNull final Class<Other> newDataType) {
        // noinspection unchecked
        return dataType == newDataType
                ? (ColumnDefinition<Other>) this
                // Do not pass along the existing component-type; it will be inferred
                : fromGenericType(name, newDataType, null, columnType);
    }

    public <Other> ColumnDefinition<Other> withDataType(
            @NotNull final Class<Other> newDataType,
            @Nullable final Class<?> newComponentType) {
        // noinspection unchecked
        return dataType == newDataType && componentType == newComponentType
                ? (ColumnDefinition<Other>) this
                : fromGenericType(name, newDataType, newComponentType, columnType);
    }

    public ColumnDefinition<?> withName(@NotNull final String newName) {
        return newName.equals(name) ? this : create(newName, dataType, componentType, columnType);
    }

    public boolean isPartitioning() {
        return (columnType == ColumnType.Partitioning);
    }

    public boolean isDirect() {
        return (columnType == ColumnType.Normal);
    }

    /**
     * Compares two ColumnDefinitions somewhat more permissively than equals, disregarding matters of storage and
     * derivation. Checks for equality of {@code name}, {@code dataType}, and {@code componentType}. As such, this
     * method has an equivalence relation, ie {@code A.isCompatible(B) == B.isCompatible(A)}.
     *
     * @param other The ColumnDefinition to compare to
     * @return Whether the ColumnDefinition defines a column whose name and data are compatible with this
     *         ColumnDefinition
     */
    public boolean isCompatible(@NotNull final ColumnDefinition<?> other) {
        if (this == other) {
            return true;
        }
        return this.name.equals(other.name)
                && this.dataType == other.dataType
                && this.componentType == other.componentType;
    }

    /**
     * Compares two ColumnDefinitions somewhat more permissively than equals, disregarding matters of name, storage and
     * derivation. Checks for equality of {@code dataType}, and {@code componentType}. As such, this method has an
     * equivalence relation, ie {@code A.hasCompatibleDataType(B) == B.hasCompatibleDataType(A)}.
     *
     * @param other - The ColumnDefinition to compare to.
     * @return True if the ColumnDefinition defines a column whose data is compatible with this ColumnDefinition.
     */
    public boolean hasCompatibleDataType(@NotNull final ColumnDefinition<?> other) {
        return dataType == other.dataType && componentType == other.componentType;
    }

    /**
     * Describes the column definition with respect to the fields that are checked in
     * {@link #isCompatible(ColumnDefinition)}.
     *
     * @return the description for compatibility
     */
    public String describeForCompatibility() {
        if (componentType == null) {
            return String.format("[%s, %s]", name, dataType);
        }
        return String.format("[%s, %s, %s]", name, dataType, componentType);
    }

    /**
     * Enumerate the differences between this ColumnDefinition, and another one. Lines will be of the form "lhs
     * attribute 'value' does not match rhs attribute 'value'.
     *
     * @param differences an array to which differences can be added
     * @param other the ColumnDefinition under comparison
     * @param lhs what to call "this" definition
     * @param rhs what to call the other definition
     * @param prefix begin each difference with this string
     * @param includeColumnType whether to include {@code columnType} comparisons
     */
    public void describeDifferences(@NotNull List<String> differences, @NotNull final ColumnDefinition<?> other,
            @NotNull final String lhs, @NotNull final String rhs, @NotNull final String prefix,
            final boolean includeColumnType) {
        if (this == other) {
            return;
        }
        if (!name.equals(other.name)) {
            differences.add(prefix + lhs + " name '" + name + "' does not match " + rhs + " name '" + other.name + "'");
        }
        if (dataType != other.dataType) {
            differences.add(prefix + lhs + " dataType '" + dataType + "' does not match " + rhs + " dataType '"
                    + other.dataType + "'");
        } else {
            if (componentType != other.componentType) {
                differences.add(prefix + lhs + " componentType '" + componentType + "' does not match " + rhs
                        + " componentType '" + other.componentType + "'");
            }
            if (includeColumnType && columnType != other.columnType) {
                differences.add(prefix + lhs + " columnType " + columnType + " does not match " + rhs + " columnType "
                        + other.columnType);
            }
        }
    }

    /**
     * Checks if objects of type {@link #getDataType() dataType} can be cast to {@code destDataType} (equivalent to
     * {@code destDataType.isAssignableFrom(dataType)}). If not, this throws a {@link ClassCastException}.
     *
     * @param destDataType the destination data type
     */
    public final void checkCastTo(Class<?> destDataType) {
        TypeHelper.checkCastTo("[" + name + "]", dataType, destDataType);
    }

    /**
     * Checks if objects of type {@link #getDataType() dataType} can be cast to {@code destDataType} (equivalent to
     * {@code destDataType.isAssignableFrom(dataType)}) and checks that objects of type {@link #getComponentType()
     * componentType} can be cast to {@code destComponentType} (both component types must be present and cast-able, or
     * both must be {@code null}; when both present, is equivalent to
     * {@code destComponentType.isAssignableFrom(componentType)}). If not, this throws a {@link ClassCastException}.
     *
     * @param destDataType the destination data type
     * @param destComponentType the destination component type, may be {@code null}
     */
    public final void checkCastTo(Class<?> destDataType, @Nullable Class<?> destComponentType) {
        TypeHelper.checkCastTo("[" + name + "]", dataType, componentType, destDataType, destComponentType);
    }

    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof ColumnDefinition)) {
            return false;
        }
        final ColumnDefinition<?> otherCD = (ColumnDefinition<?>) other;
        return name.equals(otherCD.name)
                && dataType == otherCD.dataType
                && componentType == otherCD.componentType
                && columnType == otherCD.columnType;
    }

    @Override
    public int hashCode() {
        return (((31
                + name.hashCode()) * 31
                + dataType.hashCode()) * 31
                + Objects.hashCode(componentType)) * 31
                + columnType.hashCode();
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("ColumnDefinition {")
                .append("name=").append(name)
                .append(", dataType=").append(String.valueOf(dataType))
                .append(", componentType=").append(String.valueOf(componentType))
                .append(", columnType=").append(columnType.name())
                .append('}');
    }
}
