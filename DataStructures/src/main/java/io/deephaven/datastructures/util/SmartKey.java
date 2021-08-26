/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.datastructures.util;

import io.deephaven.base.CompareUtils;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;

public class SmartKey implements Serializable, Comparable {

    private static final long serialVersionUID = -1543127380480080565L;

    public static final SmartKey EMPTY = new SmartKey(CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY);

    public Object[] values_;

    private int hashCode_;

    public SmartKey(Object... values) {
        values_ = values;

        hashCode_ = HashCodeUtil.createHashCode(values_);
    }

    @Override
    public int hashCode() {
        return hashCode_;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SmartKey)) {
            return false;
        }
        SmartKey criteria = (SmartKey) obj;

        return Arrays.equals(values_, criteria.values_);
    }

    @Override
    public String toString() {
        return "{SmartKey: values:" + Arrays.toString(values_) + " hashCode:" + hashCode_ + "}";
    }

    public int compareTo(@NotNull Object o) {
        SmartKey otherKey = (SmartKey) o;
        int activeLengthIndex = Math.min(values_.length, otherKey.values_.length);

        for (int i = 0; i < activeLengthIndex; i++) {
            int ret = CompareUtils.compare(values_[i], otherKey.values_[i]);
            if (ret != 0) {
                return ret;
            }
        }

        if (values_.length != otherKey.values_.length) {
            return values_.length < otherKey.values_.length ? -1 : 1;
        }

        return 0;
    }

    public SmartKey subKey(int... positions) {
        Object[] newValues = new Object[positions.length];
        for (int ii = 0; ii < positions.length; ++ii) {
            newValues[ii] = values_[positions[ii]];
        }
        return new SmartKey(newValues);
    }

    public int size() {
        return values_.length;
    }

    public Object get(int position) {
        return values_[position];
    }

    // A bit of nastiness and interface pollution so we can reuse the same key and array in a lower
    // garbage way
    public void updateHashCode() {
        hashCode_ = HashCodeUtil.createHashCode(values_);
    }
}
