/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Float} type.
 */
@Immutable
@SingletonStyle
public abstract class BoxedFloatType extends BoxedTypeBase<Float> {

    public static BoxedFloatType of() {
        return ImmutableBoxedFloatType.of();
    }

    @Override
    public final Class<Float> clazz() {
        return Float.class;
    }

    @Override
    public final FloatType primitiveType() {
        return FloatType.of();
    }

    @Override
    public final <R> R walk(BoxedType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
