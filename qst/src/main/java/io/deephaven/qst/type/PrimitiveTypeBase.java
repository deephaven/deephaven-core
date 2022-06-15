/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

public abstract class PrimitiveTypeBase<T> extends ColumnTypeBase<T> implements PrimitiveType<T> {

    @Override
    public final <V extends Type.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
