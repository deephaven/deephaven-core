package io.deephaven.qst.type;

import io.deephaven.qst.type.BoxedType.Visitor;

import java.util.Objects;

class PrimitiveTypeVisitorAdapter<R> implements PrimitiveType.Visitor<R> {
    private final BoxedType.Visitor<R> delegate;

    public PrimitiveTypeVisitorAdapter(Visitor<R> delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public R visit(BooleanType booleanType) {
        return delegate.visit(booleanType.boxedType());
    }

    @Override
    public R visit(ByteType byteType) {
        return delegate.visit(byteType.boxedType());
    }

    @Override
    public R visit(CharType charType) {
        return delegate.visit(charType.boxedType());
    }

    @Override
    public R visit(ShortType shortType) {
        return delegate.visit(shortType.boxedType());
    }

    @Override
    public R visit(IntType intType) {
        return delegate.visit(intType.boxedType());
    }

    @Override
    public R visit(LongType longType) {
        return delegate.visit(longType.boxedType());
    }

    @Override
    public R visit(FloatType floatType) {
        return delegate.visit(floatType.boxedType());
    }

    @Override
    public R visit(DoubleType doubleType) {
        return delegate.visit(doubleType.boxedType());
    }
}
