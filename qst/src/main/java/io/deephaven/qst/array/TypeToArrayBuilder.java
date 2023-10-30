/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.array;

import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;

class TypeToArrayBuilder implements Type.Visitor<ArrayBuilder<?, ?, ?>>, PrimitiveType.Visitor<ArrayBuilder<?, ?, ?>>,
        GenericType.Visitor<ArrayBuilder<?, ?, ?>> {

    static <T> ArrayBuilder<T, ?, ?> of(Type<T> type, int initialCapacity) {
        // noinspection unchecked
        return (ArrayBuilder<T, ?, ?>) type.walk(new TypeToArrayBuilder(initialCapacity));
    }

    static <T> ArrayBuilder<T, ? extends PrimitiveArray<T>, ?> of(PrimitiveType<T> type,
            int initialCapacity) {
        // noinspection unchecked
        return (ArrayBuilder<T, ? extends PrimitiveArray<T>, ?>) type
                .walk((PrimitiveType.Visitor<ArrayBuilder<?, ?, ?>>) new TypeToArrayBuilder(initialCapacity));
    }

    private final int initialCapacity;

    private TypeToArrayBuilder(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(PrimitiveType<?> primitiveType) {
        return primitiveType.walk((PrimitiveType.Visitor<ArrayBuilder<?, ?, ?>>) this);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(GenericType<?> genericType) {
        return genericType.walk((GenericType.Visitor<ArrayBuilder<?, ?, ?>>) this);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(BooleanType booleanType) {
        return BooleanArray.builder(initialCapacity);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(ByteType byteType) {
        return ByteArray.builder(initialCapacity);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(CharType charType) {
        return CharArray.builder(initialCapacity);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(ShortType shortType) {
        return ShortArray.builder(initialCapacity);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(IntType intType) {
        return IntArray.builder(initialCapacity);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(LongType longType) {
        return LongArray.builder(initialCapacity);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(FloatType floatType) {
        return FloatArray.builder(initialCapacity);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(DoubleType doubleType) {
        return DoubleArray.builder(initialCapacity);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(BoxedType<?> boxedType) {
        // Special case for boxed types, use the primitive type equivalent
        return visit(boxedType.primitiveType());
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(StringType stringType) {
        return GenericArray.builder(stringType);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(InstantType instantType) {
        return GenericArray.builder(instantType);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(ArrayType<?, ?> arrayType) {
        return GenericArray.builder(arrayType);
    }

    @Override
    public ArrayBuilder<?, ?, ?> visit(CustomType<?> customType) {
        return GenericArray.builder(customType);
    }
}
