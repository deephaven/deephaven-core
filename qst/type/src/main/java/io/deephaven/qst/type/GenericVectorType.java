//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class GenericVectorType<T, ComponentType> extends ArrayTypeBase<T, ComponentType> {

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
}
