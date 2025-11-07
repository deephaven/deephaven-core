//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * A type.
 *
 * @param <T> the type
 * @see PrimitiveType
 * @see GenericType
 */
public interface Type<T> {

    /**
     * Finds the {@link #knownTypes() known type}, or else creates the relevant {@link NativeArrayType native array
     * type} or {@link CustomType custom type}.
     *
     * @param dataType the data type
     * @param <T> the generic type of {@code dataType}
     * @return the type
     */
    static <T> Type<T> find(Class<T> dataType) {
        Optional<Type<T>> found = TypeHelper.findStatic(dataType);
        if (found.isPresent()) {
            return found.get();
        }
        if (dataType.isArray()) {
            return NativeArrayType.of(dataType, find(dataType.getComponentType()));
        }
        return CustomType.of(dataType);
    }

    /**
     * If {@code componentType} is not {@code null}, this will find the appropriate {@link ArrayType}. Otherwise, this
     * is equivalent to {@link #find(Class)}.
     *
     * @param dataType the data type
     * @param componentType the component type
     * @return the type
     * @param <T> the generic type of {@code dataType}
     */
    static <T> Type<T> find(final Class<T> dataType, @Nullable final Class<?> componentType) {
        if (componentType == null) {
            return find(dataType);
        }
        final Type<?> ct = find(componentType);
        if (dataType.isArray()) {
            return NativeArrayType.of(dataType, ct);
        }
        if (componentType.isPrimitive()) {
            return PrimitiveVectorType.of(dataType, (PrimitiveType<?>) ct);
        }
        return GenericVectorType.of(dataType, (GenericType<?>) ct);
    }

    /**
     * The list of known types. Includes the universe of {@link PrimitiveType primitive types} and {@link GenericType
     * generic types} minus {@link CustomType custom types} and {@link ArrayType array types}.
     *
     * @return the list of known types
     */
    static List<Type<?>> knownTypes() {
        return TypeHelper.knownTypes();
    }

    /**
     * Creates the boolean type.
     *
     * @return the boolean type
     */
    static BooleanType booleanType() {
        return BooleanType.of();
    }

    /**
     * Creates the byte type.
     *
     * @return the byte type
     */
    static ByteType byteType() {
        return ByteType.of();
    }

    /**
     * Creates the char type.
     *
     * @return the char type
     */
    static CharType charType() {
        return CharType.of();
    }

    /**
     * Creates the short type.
     *
     * @return the short type
     */
    static ShortType shortType() {
        return ShortType.of();
    }

    /**
     * Creates the int type.
     *
     * @return the int type
     */
    static IntType intType() {
        return IntType.of();
    }

    /**
     * Creates the long type.
     *
     * @return the long type
     */
    static LongType longType() {
        return LongType.of();
    }

    /**
     * Creates the float type.
     *
     * @return the float type
     */
    static FloatType floatType() {
        return FloatType.of();
    }

    /**
     * Creates the double type.
     *
     * @return the double type
     */
    static DoubleType doubleType() {
        return DoubleType.of();
    }

    /**
     * Creates the string type.
     *
     * @return the string type
     */
    static StringType stringType() {
        return StringType.of();
    }

    /**
     * Creates the instant type.
     *
     * @return the instant type
     */
    static InstantType instantType() {
        return InstantType.of();
    }

    /**
     * Creates a custom type. Equivalent to {@code CustomType.of(clazz)}.
     *
     * @param clazz the class
     * @param <T> the type
     * @return the custom type
     */
    static <T> CustomType<T> ofCustom(Class<T> clazz) {
        return CustomType.of(clazz);
    }

    /**
     * The class representing {@code this} type.
     *
     * @return the class
     */
    Class<T> clazz();

    /**
     * Create a {@link NativeArrayType native array type} with {@code this} as the component type.
     *
     * @return the native array type
     */
    NativeArrayType<?, T> arrayType();

    <R> R walk(Visitor<R> visitor);

    interface Visitor<R> {
        R visit(PrimitiveType<?> primitiveType);

        R visit(GenericType<?> genericType);

        // Implementation note: when adding new types here, add type to TypeHelper
    }
}
