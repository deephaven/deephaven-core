/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class GenericVectorType<T, ComponentType> extends ArrayTypeBase<T, ComponentType> {

    private static final String OBJECT_VECTOR = "io.deephaven.vector.ObjectVector";

    public static <T, ComponentType> GenericVectorType<T, ComponentType> of(
            Class<T> clazz,
            GenericType<ComponentType> componentType) {
        return ImmutableGenericVectorType.of(clazz, componentType);
    }

    @Parameter
    public abstract Class<T> clazz();

    @Parameter
    public abstract GenericType<ComponentType> componentType();

    @Override
    public final <R> R walk(ArrayType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkClazz() {
        final Class<?> baseClass;
        try {
            baseClass = Class.forName(OBJECT_VECTOR, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
        if (!baseClass.isAssignableFrom(clazz())) {
            throw new IllegalArgumentException(
                    String.format("Expected %s to be assignable to %s", clazz().getName(), OBJECT_VECTOR));
        }
    }
}
