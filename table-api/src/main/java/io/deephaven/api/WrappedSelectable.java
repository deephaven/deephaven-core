//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.api.expression.Expression;

import java.util.Arrays;

class WrappedSelectable implements Selectable {
    Selectable wrapped;

    WrappedSelectable(Selectable wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public ColumnName newColumn() {
        return wrapped.newColumn();
    }

    @Override
    public Expression expression() {
        return wrapped.expression();
    }

    @Override
    public Object[] respectedBarriers() {
        return wrapped.respectedBarriers();
    }

    @Override
    public Object[] declaredBarriers() {
        return wrapped.declaredBarriers();
    }

    @Override
    public Boolean isSerial() {
        return wrapped.isSerial();
    }
}
