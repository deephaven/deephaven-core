package io.deephaven.qst.type;

import io.deephaven.qst.type.PrimitiveType.Visitor;

import java.util.Objects;

class BoxedTypeVisitorAdapter<R> implements BoxedType.Visitor<R> {
    private final PrimitiveType.Visitor<R> delegate;

    public BoxedTypeVisitorAdapter(Visitor<R> delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public R visit(BoxedBooleanType booleanType) {
        return delegate.visit(booleanType.primitiveType());
    }

    @Override
    public R visit(BoxedByteType byteType) {
        return delegate.visit(byteType.primitiveType());
    }

    @Override
    public R visit(BoxedCharType charType) {
        return delegate.visit(charType.primitiveType());
    }

    @Override
    public R visit(BoxedShortType shortType) {
        return delegate.visit(shortType.primitiveType());
    }

    @Override
    public R visit(BoxedIntType intType) {
        return delegate.visit(intType.primitiveType());
    }

    @Override
    public R visit(BoxedLongType longType) {
        return delegate.visit(longType.primitiveType());
    }

    @Override
    public R visit(BoxedFloatType floatType) {
        return delegate.visit(floatType.primitiveType());
    }

    @Override
    public R visit(BoxedDoubleType doubleType) {
        return delegate.visit(doubleType.primitiveType());
    }
}
