/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link String} type.
 */
@Immutable
@SingletonStyle
public abstract class StringType extends GenericTypeBase<String> {

    public static StringType of() {
        return ImmutableStringType.of();
    }

    @Override
    public final Class<String> clazz() {
        return String.class;
    }

    @Override
    public final <R> R walk(GenericType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return StringType.class.getName();
    }
}
