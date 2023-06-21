/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * The {@link Float} type.
 */
@Immutable
@SimpleStyle
public abstract class FloatType extends PrimitiveTypeBase<Float> {

    public static FloatType instance() {
        return ImmutableFloatType.of();
    }

    @Override
    public final Class<Float> clazz() {
        return float.class;
    }

    @Override
    public final Class<Float> boxedClass() {
        return Float.class;
    }

    @Override
    public final NativeArrayType<float[], Float> arrayType() {
        return NativeArrayType.of(float[].class, this);
    }

    @Override
    public final <R> R walk(PrimitiveType.Visitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public final String toString() {
        return FloatType.class.getName();
    }
}
