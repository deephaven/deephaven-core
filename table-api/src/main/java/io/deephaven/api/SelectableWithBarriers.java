//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.api.expression.Expression;
import io.deephaven.base.ArrayUtil;

class SelectableWithBarriers implements Selectable {
    Selectable wrapped;
    Object[] barriers;

    SelectableWithBarriers(Selectable wrapped, final Object[] barriers) {
        this.wrapped = wrapped;
        if (wrapped.barriers() != null && wrapped.barriers().length > 0) {
            this.barriers = ArrayUtil.concat(barriers, wrapped.barriers());
        } else {
            this.barriers = barriers;
        }
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
        return barriers;
    }

    @Override
    public Boolean isSerial() {
        return wrapped.isSerial();
    }

    @Override
    public Selectable withSerial() {
        return new SelectableWithSerial(this);
    }

    @Override
    public Selectable withBarriers(Object... barriers) {
        return new SelectableWithBarriers(this, barriers);
    }

    @Override
    public Selectable respectsBarriers(Object... barriers) {
        return new SelectableWithBarriers(this, barriers);
    }
}
