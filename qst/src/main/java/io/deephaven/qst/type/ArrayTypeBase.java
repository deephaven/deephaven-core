/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.type;

public abstract class ArrayTypeBase<T, ComponentType> extends GenericTypeBase<T>
        implements ArrayType<T, ComponentType> {

    @Override
    public final <V extends GenericType.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
