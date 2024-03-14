//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base;

import junit.framework.TestCase;

public class MathUtilTest extends TestCase {

    public void testGCD() {
        check(0, 0, 0);
        check(0, 1, 1);
        check(1, 1, 1);
        check(2, 1, 1);
        check(3, 2, 1);
        check(9, 6, 3);
        check(9, 9, 9);
        check(100, 1, 1);
    }

    public void check(int a, int b, int expect) {
        assertEquals(expect, MathUtil.gcd(a, b));
        assertEquals(expect, MathUtil.gcd(b, a));
        assertEquals(expect, MathUtil.gcd(-a, -b));
        assertEquals(expect, MathUtil.gcd(-b, -a));
    }
}
