/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.testing;

import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

// --------------------------------------------------------------------
/**
 * Helper to do a heuristic check for really poor hash code implementations.
 */
public class HashCodeChecker {
    private int m_nCount;
    private final Set<Integer> m_hashCodes = new HashSet<Integer>();

    public void addHashCodeOfObject(Object o) {
        m_hashCodes.add(o.hashCode());
        m_nCount++;
    }

    public void check() {
        Assert.assertTrue(m_hashCodes.size() > m_nCount * (.75));
    }
}
