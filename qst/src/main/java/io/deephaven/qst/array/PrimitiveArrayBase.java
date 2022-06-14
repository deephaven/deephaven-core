/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.array;

public abstract class PrimitiveArrayBase<T> implements PrimitiveArray<T> {

    @Override
    public final <V extends Array.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
