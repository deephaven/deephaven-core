//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;


import java.io.Serializable;

public abstract class WhereFilterImpl implements WhereFilter, Serializable {
    private boolean isAutomatedFilter = false;

    @Override
    public boolean isAutomatedFilter() {
        return isAutomatedFilter;
    }

    @Override
    public void setAutomatedFilter(boolean value) {
        isAutomatedFilter = value;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visitOther(this);
    }
}
