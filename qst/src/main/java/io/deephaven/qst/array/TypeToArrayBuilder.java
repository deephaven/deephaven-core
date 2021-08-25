package io.deephaven.qst.array;

import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.Type;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;

import java.util.Objects;

class TypeToArrayBuilder implements Type.Visitor, PrimitiveType.Visitor {

    static <T> ArrayBuilder<T, ?, ?> of(Type<T> type, int initialCapacity) {
        // noinspection unchecked
        return (ArrayBuilder<T, ?, ?>) type.walk(new TypeToArrayBuilder(initialCapacity)).getOut();
    }

    static <T> ArrayBuilder<T, ? extends PrimitiveArray<T>, ?> of(PrimitiveType<T> type,
            int initialCapacity) {
        TypeToArrayBuilder visitor = new TypeToArrayBuilder(initialCapacity);
        type.walk((PrimitiveType.Visitor) visitor);
        // noinspection unchecked
        return (ArrayBuilder<T, ? extends PrimitiveArray<T>, ?>) visitor.getOut();
    }

    private final int initialCapacity;
    private ArrayBuilder<?, ?, ?> out;

    private TypeToArrayBuilder(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    public ArrayBuilder<?, ?, ?> getOut() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(PrimitiveType<?> primitiveType) {
        primitiveType.walk((PrimitiveType.Visitor) this);
    }

    @Override
    public void visit(GenericType<?> genericType) {
        out = GenericArray.builder(genericType);
    }

    @Override
    public void visit(BooleanType booleanType) {
        out = BooleanArray.builder(initialCapacity);
    }

    @Override
    public void visit(ByteType byteType) {
        out = ByteArray.builder(initialCapacity);
    }

    @Override
    public void visit(CharType charType) {
        out = CharArray.builder(initialCapacity);
    }

    @Override
    public void visit(ShortType shortType) {
        out = ShortArray.builder(initialCapacity);
    }

    @Override
    public void visit(IntType intType) {
        out = IntArray.builder(initialCapacity);
    }

    @Override
    public void visit(LongType longType) {
        out = LongArray.builder(initialCapacity);
    }

    @Override
    public void visit(FloatType floatType) {
        out = FloatArray.builder(initialCapacity);
    }

    @Override
    public void visit(DoubleType doubleType) {
        out = DoubleArray.builder(initialCapacity);
    }
}
