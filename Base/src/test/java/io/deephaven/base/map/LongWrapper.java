/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.map;

import io.deephaven.base.Copyable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

class LongWrapper implements Comparable<LongWrapper>, Externalizable, Copyable<LongWrapper> {
    long val;

    public LongWrapper() {
        val = Long.MIN_VALUE;
    }

    LongWrapper(Long val) {
        this.val = val;
    }

    public long getVal() {
        return val;
    }

    @Override
    public int compareTo(LongWrapper o) {
        if (val < o.val) {
            return -1;
        } else if (val > o.val) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(val);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        val = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof LongWrapper))
            return false;

        LongWrapper that = (LongWrapper) o;

        if (val != that.val)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (val ^ (val >>> 32));
    }

    @Override
    public String toString() {
        return "LongWrapper: " + val;
    }

    @Override
    public void copyValues(LongWrapper other) {
        val = other.val;
    }

    @Override
    public LongWrapper clone() {
        return new LongWrapper(val);
    }

    @Override
    public LongWrapper safeClone() {
        return clone();
    }
}
