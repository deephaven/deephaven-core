/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.integrations.pyserver.wrapper;

import java.util.Objects;

/**
 * See test_wrapper.py
 */
@SuppressWarnings("unused")
public final class MyObject implements Comparable<MyObject> {
    private final int hash;
    private final String str;

    public MyObject(int hash, String str) {
        this.hash = hash;
        this.str = Objects.requireNonNull(str);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        MyObject myEnum = (MyObject) o;
        if (hash != myEnum.hash)
            return false;
        return str.equals(myEnum.str);
    }

    @Override
    public int compareTo(MyObject o) {
        final int c1 = Integer.compare(hash, o.hash);
        if (c1 != 0) {
            return c1;
        }
        return str.compareTo(o.str);
    }

    @Override
    public String toString() {
        return str;
    }
}
