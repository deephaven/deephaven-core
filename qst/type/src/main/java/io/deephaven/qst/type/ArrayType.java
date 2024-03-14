//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

/**
 * An array-like type.
 *
 * @param <T> the array type representing {@code this}
 * @param <ComponentType> the component type
 * @see NativeArrayType
 * @see PrimitiveVectorType
 * @see GenericVectorType
 */
public interface ArrayType<T, ComponentType> extends GenericType<T> {

    /**
     * The component type.
     *
     * @return the component type
     */
    Type<ComponentType> componentType();

    <R> R walk(Visitor<R> visitor);

    interface Visitor<R> {
        R visit(NativeArrayType<?, ?> nativeArrayType);

        R visit(PrimitiveVectorType<?, ?> vectorPrimitiveType);

        R visit(GenericVectorType<?, ?> genericVectorType);
    }
}
