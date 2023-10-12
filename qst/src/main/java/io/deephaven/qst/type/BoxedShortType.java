/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Short} type.
 */
@Immutable
@SingletonStyle
public abstract class BoxedShortType extends BoxedTypeBase<Short> {

    public static BoxedShortType of() {
        return ImmutableBoxedShortType.of();
    }

    @Override
    public final Class<Short> clazz() {
        return Short.class;
    }

    @Override
    public final ShortType primitiveType() {
        return ShortType.of();
    }

    @Override
    public final <R> R walk(BoxedType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
