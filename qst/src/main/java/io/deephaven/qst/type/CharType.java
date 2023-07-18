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
public abstract class CharType extends PrimitiveTypeBase<Character> {

    public static CharType instance() {
        return ImmutableCharType.of();
    }

    @Override
    public final Class<Character> clazz() {
        return char.class;
    }

    @Override
    public final Class<Character> boxedClass() {
        return Character.class;
    }

    @Override
    public final NativeArrayType<char[], Character> arrayType() {
        return NativeArrayType.of(char[].class, this);
    }

    @Override
    public final <R> R walk(PrimitiveType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return CharType.class.getName();
    }
}
