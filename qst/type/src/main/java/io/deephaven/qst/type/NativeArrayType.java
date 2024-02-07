/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.lang.reflect.Array;

/**
 * A java native array type.
 *
 * @param <T> the array type representing {@code this}
 * @param <ComponentType> the component type
 */
@Immutable
@SimpleStyle
public abstract class NativeArrayType<T, ComponentType> extends ArrayTypeBase<T, ComponentType> {

    public static <T> NativeArrayType<T, ?> find(Class<T> arrayType) {
        if (!arrayType.isArray()) {
            throw new IllegalArgumentException("arrayType must be an array type");
        }
        return of(arrayType, Type.find(arrayType.getComponentType()));
    }

    public static <T, ComponentType> NativeArrayType<T, ComponentType> of(Class<T> arrayType,
            Type<ComponentType> componentType) {
        return ImmutableNativeArrayType.of(arrayType, componentType);
    }

    public static <ComponentType> NativeArrayType<ComponentType[], ComponentType> toArrayType(
            GenericType<ComponentType> type) {
        // Note: in Java 12+, we can use Class#arrayType()
        final Class<?> clazz = Array.newInstance(type.clazz(), 0).getClass();
        // noinspection unchecked
        return (NativeArrayType<ComponentType[], ComponentType>) NativeArrayType.of(clazz, type);
    }

    @Parameter
    public abstract Class<T> clazz();

    @Parameter
    public abstract Type<ComponentType> componentType();

    @Override
    public final <R> R walk(ArrayType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkArrayType() {
        if (!clazz().isArray()) {
            throw new IllegalArgumentException("clazz() must be an array type");
        }
        Class<?> componentType = clazz().getComponentType();
        if (!componentType.equals(componentType().clazz())) {
            throw new IllegalArgumentException("Component types don't match");
        }
    }
}
