//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The primitive {@link char} type.
 */
@Immutable
@SingletonStyle
public abstract class CharType extends PrimitiveTypeBase<Character> {

    public static CharType of() {
        return ImmutableCharType.of();
    }

    @Override
    public final Class<Character> clazz() {
        return char.class;
    }

    @Override
    public final BoxedCharType boxedType() {
        return BoxedCharType.of();
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
