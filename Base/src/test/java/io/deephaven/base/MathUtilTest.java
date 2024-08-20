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

    public void testRoundUpPowerOf2() {
        pow2(0, 1);
        pow2(1, 1);
        pow2(2, 2);
        for (int i = 2; i < 31; ++i) {
            final int pow2 = 1 << i;
            pow2(pow2, pow2);
            pow2(pow2 - 1, pow2);
            if (i < 30) {
                pow2(pow2 + 1, pow2 * 2);
            }
        }
    }

    public void testRoundUpArraySize() {
        arraySize(0, 1);
        arraySize(1, 1);
        arraySize(2, 2);
        for (int i = 2; i < 31; ++i) {
            final int pow2 = 1 << i;
            arraySize(pow2, pow2);
            arraySize(pow2 - 1, pow2);
            if (i < 30) {
                arraySize(pow2 + 1, pow2 * 2);
            } else {
                arraySize(pow2 + 1, ArrayUtil.MAX_ARRAY_SIZE);
            }
        }
        arraySize(Integer.MAX_VALUE, ArrayUtil.MAX_ARRAY_SIZE);
    }

    public static void pow2(int newSize, int expectedSize) {
        assertEquals(MathUtil.roundUpPowerOf2(newSize), expectedSize);
    }

    public static void arraySize(int newSize, int expectedSize) {
        assertEquals(MathUtil.roundUpArraySize(newSize), expectedSize);
    }
}
