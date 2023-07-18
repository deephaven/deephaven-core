/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import java.util.stream.Stream;

public interface BoxedType<T> extends GenericType<T> {

    static Stream<BoxedType<?>> instances() {
        return Stream.of(
                BoxedBooleanType.of(),
                BoxedByteType.of(),
                BoxedCharType.of(),
                BoxedShortType.of(),
                BoxedIntType.of(),
                BoxedLongType.of(),
                BoxedFloatType.of(),
                BoxedDoubleType.of());
    }

    PrimitiveType<T> primitiveType();

    <R> R walk(Visitor<R> visitor);

    interface Visitor<R> {
        static <R> Visitor<R> adapt(PrimitiveType.Visitor<R> visitor) {
            return new BoxedTypeVisitorAdapter<>(visitor);
        }

        R visit(BoxedBooleanType booleanType);

        R visit(BoxedByteType byteType);

        R visit(BoxedCharType charType);

        R visit(BoxedShortType shortType);

        R visit(BoxedIntType intType);

        R visit(BoxedLongType longType);

        R visit(BoxedFloatType floatType);

        R visit(BoxedDoubleType doubleType);
    }
}
