/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Long} type.
 */
@Immutable
@SingletonStyle
public abstract class BoxedLongType extends BoxedTypeBase<Long> {

    public static BoxedLongType of() {
        return ImmutableBoxedLongType.of();
    }

    @Override
    public final Class<Long> clazz() {
        return Long.class;
    }

    @Override
    public final LongType primitiveType() {
        return LongType.of();
    }

    @Override
    public final <R> R walk(BoxedType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
