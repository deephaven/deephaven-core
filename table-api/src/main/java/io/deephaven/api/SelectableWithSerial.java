//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.api.expression.Expression;

import java.util.Collection;
import java.util.List;

class SelectableWithSerial implements Selectable {
    Selectable wrapped;

    SelectableWithSerial(Selectable wrapped) {
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
    public Object[] barriers() {
        return wrapped.barriers();
    }

    @Override
    public Boolean isSerial() {
        return true;
    }

    @Override
    public Selectable withSerial() {
        return this;
    }

    @Override
    public Selectable withBarriers(Object... barriers) {
        return new SelectableWithBarriers(this, barriers);
    }

    @Override
    public Selectable respectsBarriers(Object... barriers) {
        return new SelectableWithRespectsBarrier(this, barriers);
    }
}
