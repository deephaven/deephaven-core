/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SingletonStyle;
import org.immutables.value.Value.Immutable;

/**
 * The primitive {@link long} type.
 */
@Immutable
@SingletonStyle
public abstract class LongType extends PrimitiveTypeBase<Long> {

    public static LongType of() {
        return ImmutableLongType.of();
    }

    @Override
    public final Class<Long> clazz() {
        return long.class;
    }

    @Override
    public final BoxedLongType boxedType() {
        return BoxedLongType.of();
    }

    @Override
    public final NativeArrayType<long[], Long> arrayType() {
        return NativeArrayType.of(long[].class, this);
    }

    @Override
    public final <R> R walk(PrimitiveType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return LongType.class.getName();
    }
}
