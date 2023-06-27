/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class BoxedType<T> extends GenericTypeBase<T> {

    public static <T> BoxedType<T> of(PrimitiveType<T> primitiveType) {
        return ImmutableBoxedType.of(primitiveType);
    }

    @Parameter
    public abstract PrimitiveType<T> primitiveType();

    @Override
    public final Class<T> clazz() {
        return primitiveType().boxedClass();
    }

    @Override
    public final <R> R walk(GenericType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
