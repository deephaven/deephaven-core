package io.deephaven.db.v2.select;


import java.io.Serializable;

public abstract class SelectFilterImpl implements SelectFilter, Serializable {
    private boolean isAutomatedFilter = false;

    @Override
    public boolean isAutomatedFilter() {
        return isAutomatedFilter;
    }

    @Override
    public void setAutomatedFilter(boolean value) {
        isAutomatedFilter = value;
    }
}
