//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Double} type.
 */
@Immutable
@SingletonStyle
public abstract class BoxedDoubleType extends BoxedTypeBase<Double> {

    public static BoxedDoubleType of() {
        return ImmutableBoxedDoubleType.of();
    }

    @Override
    public final Class<Double> clazz() {
        return Double.class;
    }

    @Override
    public final DoubleType primitiveType() {
        return DoubleType.of();
    }

    @Override
    public final <R> R walk(BoxedType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
