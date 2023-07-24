/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Integer} type.
 */
@Immutable
@SingletonStyle
public abstract class BoxedIntType extends BoxedTypeBase<Integer> {

    public static BoxedIntType of() {
        return ImmutableBoxedIntType.of();
    }

    @Override
    public final Class<Integer> clazz() {
        return Integer.class;
    }

    @Override
    public final IntType primitiveType() {
        return IntType.of();
    }

    @Override
    public final <R> R walk(BoxedType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
