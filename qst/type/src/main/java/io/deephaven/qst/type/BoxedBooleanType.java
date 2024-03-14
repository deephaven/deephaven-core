//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Boolean} type.
 */
@Immutable
@SingletonStyle
public abstract class BoxedBooleanType extends BoxedTypeBase<Boolean> {

    public static BoxedBooleanType of() {
        return ImmutableBoxedBooleanType.of();
    }

    @Override
    public final Class<Boolean> clazz() {
        return Boolean.class;
    }

    @Override
    public final BooleanType primitiveType() {
        return BooleanType.of();
    }

    @Override
    public final <R> R walk(BoxedType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
