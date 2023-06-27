/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

public abstract class PrimitiveTypeBase<T> extends ColumnTypeBase<T> implements PrimitiveType<T> {

    @Override
    public final BoxedType<T> boxedType() {
        return BoxedType.of(this);
    }

    @Override
    public final <R> R walk(Type.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
