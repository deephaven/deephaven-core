package io.deephaven.utils;

import junit.framework.TestCase;

import java.math.BigDecimal;

public class TestBigDecimalSqrt extends TestCase {
    public void testSqrt() {
        /*
         * x = √10; eps = 1e-11 => (x + / -eps)^2 = x^2 +/- 2*eps*x + eps^2 Therefore the sqrt squared is accurate to
         * 2*eps*x, which is ~6e-11 in this case.
         */
        testSqrt(BigDecimal.TEN, 11, 10);
        testSqrt(BigDecimal.ONE, 10, 10);
        testSqrt(BigDecimal.TEN.scaleByPowerOfTen(100), 50, 0);
        testSqrt(BigDecimal.valueOf(Long.MAX_VALUE).pow(4), 10, 10);
        testSqrt(BigDecimal.valueOf(Long.MAX_VALUE).pow(4).divide(BigDecimal.valueOf(7), BigDecimal.ROUND_HALF_UP), 40,
                0);
        // value > Double.MAX_VALUE
        testSqrt(new BigDecimal("1.3965847798346571265746871246578426578246587146581476581476578465814276518E400"), 400,
                200);
    }

    public void testSqrtExceptionals() {
        final BigDecimal sqrt = BigDecimalUtils.sqrt(null, 10);
        System.out.println("sqrt(null): " + sqrt);
        assertNull(sqrt);

        final BigDecimal value = BigDecimal.valueOf(Long.MIN_VALUE);
        int scale = 10;
        try {
            BigDecimalUtils.sqrt(value, scale);
            TestCase.fail("Expected : " + IllegalArgumentException.class);
        } catch (final IllegalArgumentException negativeValExc) {
            TestCase.assertTrue(value.signum() < 0);
        }

        scale = -10;
        try {
            BigDecimalUtils.sqrt(BigDecimal.TEN, scale);
            TestCase.fail("Expected : " + IllegalArgumentException.class);
        } catch (final IllegalArgumentException negativeScaleExc) {
            TestCase.assertTrue(negativeScaleExc.getMessage().contains("scale"));
        }
    }

    private void testSqrt(final BigDecimal value, final int scale, final int checkScale) {
        final BigDecimal sqrt = BigDecimalUtils.sqrt(value, scale);
        System.out.println("sqrt(" + value + "): " + sqrt);
        check(value, sqrt.pow(2), checkScale);
    }

    /**
     * Checks that expected is equal to value, within the precision of 10^-scale.
     */
    private void check(final BigDecimal expected, final BigDecimal actual, final int scale) {
        final BigDecimal difference = expected.subtract(actual).abs();
        if (difference.compareTo(BigDecimal.ONE.scaleByPowerOfTen(-scale)) > 0) {
            TestCase.assertEquals(expected, actual);
        }
    }
}
