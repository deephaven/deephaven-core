/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.join;

import io.deephaven.engine.table.impl.util.RowRedirection;

public abstract class AbstractJoinKeyState implements JoinKeyState {
    private int slot1;
    private int slot2;

    protected final RowRedirection rowRedirection;
    protected Object key;
    private boolean active = false;

    AbstractJoinKeyState(Object key, RowRedirection rowRedirection) {
        this.key = key;
        this.rowRedirection = rowRedirection;
    }

    public Object getKey() {
        return key;
    }

    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public void setActive() {
        this.active = true;
    }

    @Override
    public String dumpString() {
        return "{Key: " + getKey() + "}";
    }

    @Override
    public int getSlot1() {
        return slot1;
    }

    @Override
    public void setSlot1(int slot) {
        this.slot1 = slot;
    }

    @Override
    public int getSlot2() {
        return slot2;
    }

    @Override
    public void setSlot2(int slot) {
        this.slot2 = slot;
    }
}
