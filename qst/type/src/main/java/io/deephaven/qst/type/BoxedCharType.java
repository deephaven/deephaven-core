/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Character} type.
 */
@Immutable
@SingletonStyle
public abstract class BoxedCharType extends BoxedTypeBase<Character> {

    public static BoxedCharType of() {
        return ImmutableBoxedCharType.of();
    }

    @Override
    public final Class<Character> clazz() {
        return Character.class;
    }

    @Override
    public final CharType primitiveType() {
        return CharType.of();
    }

    @Override
    public final <R> R walk(BoxedType.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
