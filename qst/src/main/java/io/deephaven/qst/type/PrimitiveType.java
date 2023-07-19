/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import java.util.stream.Stream;

/**
 * A primitive type.
 *
 * @param <T> the primitive type
 * @see BooleanType
 * @see ByteType
 * @see CharType
 * @see ShortType
 * @see IntType
 * @see LongType
 * @see FloatType
 * @see DoubleType
 */
public interface PrimitiveType<T> extends Type<T> {

    static Stream<PrimitiveType<?>> instances() {
        return Stream.of(
                BooleanType.of(),
                ByteType.of(),
                CharType.of(),
                ShortType.of(),
                IntType.of(),
                LongType.of(),
                FloatType.of(),
                DoubleType.of());
    }

    BoxedType<T> boxedType();

    <R> R walk(Visitor<R> visitor);

    interface Visitor<R> {

        R visit(BooleanType booleanType);

        R visit(ByteType byteType);

        R visit(CharType charType);

        R visit(ShortType shortType);

        R visit(IntType intType);

        R visit(LongType longType);

        R visit(FloatType floatType);

        R visit(DoubleType doubleType);
    }
}
